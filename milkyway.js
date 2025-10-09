// To ensure we use native resolution of screen
var dpr = window.devicePixelRatio || 1;

// getting canvas with native resolution
const canvas = document.getElementById("starsCanvas");
canvas.width = window.innerWidth * dpr;
canvas.height = window.innerHeight * dpr;
const ctx = canvas.getContext('2d');
ctx.scale(dpr, dpr); // scale every drawing operation

// Shooting stars parameters
const shootingStarDensity = 0.01;
const shootingStarBaseXspeed = 30;
const shootingStarBaseYspeed = 15;
const shootingStarBaseLength = 8;
const shootingStarBaseLifespan = 60;

// Shooting star colors
const shootingStarsColors = [
  "#a1ffba", // greenish
  "#a1d2ff", // blueish
  "#fffaa1", // yellowish
  "#ffa1a1"  // reddish
];

// Random values array
let randomArray;
const randomArrayLength = 1000;
let randomArrayIterator = 0;

// Array containing shooting stars
let ShootingStarsArray = [];

// Shooting Star creation
class ShootingStar {
  constructor(x, y, speedX, speedY, color) {
    this.x = x;
    this.y = y;
    this.speedX = speedX;
    this.speedY = speedY;
    this.framesLeft = shootingStarBaseLifespan;
    this.color = color;
  }

  goingOut() {
    return this.framesLeft <= 0;
  }

  ageModifier() {
    let halfLife = shootingStarBaseLifespan / 2.0;
    return Math.pow(1.0 - Math.abs(this.framesLeft - halfLife) / halfLife, 2);
  }

  draw() {
    let am = this.ageModifier();
    let endX = this.x - this.speedX * shootingStarBaseLength * am;
    let endY = this.y - this.speedY * shootingStarBaseLength * am;

    let gradient = ctx.createLinearGradient(this.x, this.y, endX, endY);
    gradient.addColorStop(0, "#fff");
    gradient.addColorStop(Math.min(am, 0.7), this.color);
    gradient.addColorStop(1, "rgba(0,0,0,0)");

    ctx.strokeStyle = gradient;
    ctx.beginPath();
    ctx.moveTo(this.x, this.y);
    ctx.lineTo(endX, endY);
    ctx.stroke();
  }

  update() {
    this.framesLeft--;
    this.x += this.speedX;
    this.y += this.speedY;
    this.draw();
  }
}

function init() {
  randomArray = [];
  for (let i = 0; i < randomArrayLength; i++) {
    randomArray[i] = Math.random();
  }
  ShootingStarsArray = [];
}

function animate() {
  requestAnimationFrame(animate);

  ctx.clearRect(0, 0, innerWidth, innerHeight);

  if (randomArray[randomArrayIterator] < shootingStarDensity) {
    let posX = Math.floor(Math.random() * canvas.width);
    let posY = Math.floor(Math.random() * 150);
    let speedX = Math.floor((Math.random() - 0.5) * shootingStarBaseXspeed);
    let speedY = Math.floor(Math.random() * shootingStarBaseYspeed);
    let color = shootingStarsColors[Math.floor(Math.random() * shootingStarsColors.length)];
    ShootingStarsArray.push(new ShootingStar(posX, posY, speedX, speedY, color));
  }

  for (let i = ShootingStarsArray.length - 1; i >= 0; i--) {
    if (ShootingStarsArray[i].goingOut()) {
      ShootingStarsArray.splice(i, 1);
    } else {
      ShootingStarsArray[i].update();
    }
  }

  randomArrayIterator = (randomArrayIterator + 1) % randomArrayLength;
}

init();
animate();
